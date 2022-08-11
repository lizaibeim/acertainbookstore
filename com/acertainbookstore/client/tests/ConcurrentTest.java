package com.acertainbookstore.client.tests;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acertainbookstore.business.Book;
import com.acertainbookstore.business.BookCopy;
import com.acertainbookstore.business.SingleLockConcurrentCertainBookStore;
import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.business.TwoLevelLockingConcurrentCertainBookStore;
import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;


/**
 * {@link BookStoreTest} tests the {@link BookStore} interface.
 *
 * @see BookStore
 */
public class ConcurrentTest {

    /** The Constant TEST_ISBN. */
    private static final int TEST_ISBN = 3044560;

    /** The Constant NUM_COPIES. */
    private static final int NUM_COPIES = 5;

    /** The local test. */
    private static boolean localTest = true;

    /** Single lock test */
    private static boolean singleLock = true;


    /** The store manager. */
    private static StockManager storeManager;

    /** The client. */
    private static BookStore client;

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void setUpBeforeClass() {
        try {
            String localTestProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_LOCAL_TEST);
            localTest = (localTestProperty != null) ? Boolean.parseBoolean(localTestProperty) : localTest;
            String singleLockProperty = System.getProperty(BookStoreConstants.PROPERTY_KEY_SINGLE_LOCK);
            singleLock = (singleLockProperty != null) ? Boolean.parseBoolean(singleLockProperty) : singleLock;
            singleLock = false;

            if (localTest) {
                if (singleLock) {
                    SingleLockConcurrentCertainBookStore store = new SingleLockConcurrentCertainBookStore();
                    storeManager = store;
                    client = store;
                } else {
                    TwoLevelLockingConcurrentCertainBookStore store = new TwoLevelLockingConcurrentCertainBookStore();
                    storeManager = store;
                    client = store;
                }
            } else {
                storeManager = new StockManagerHTTPProxy("http://localhost:8081/stock");
                client = new BookStoreHTTPProxy("http://localhost:8081");
            }

            storeManager.removeAllBooks();
            storeManager.releaseAllLocks();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper method to add some books.
     *
     * @param isbn
     *            the isbn
     * @param copies
     *            the copies
     * @throws BookStoreException
     *             the book store exception
     */
    public void addBooks(int isbn, int copies) throws BookStoreException {
        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        StockBook book = new ImmutableStockBook(isbn, "Test of Thrones", "George RR Testin'", (float) 10, copies, 0, 0,
                0, false);
        booksToAdd.add(book);
        storeManager.addBooks(booksToAdd);
        storeManager.releaseAllLocks();
    }

    /**
     * Helper method to get the default book used by initializeBooks.
     *
     * @return the default book
     */
    public StockBook getDefaultBook() {
        return new ImmutableStockBook(TEST_ISBN, "Harry Potter and JUnit", "JK Unit", (float) 10, NUM_COPIES, 0, 0, 0,
                false);
    }

    /**
     * Method to add a book, executed before every test case is run.
     *
     * @throws BookStoreException
     *             the book store exception
     */
    @Before
    public void initializeBooks() throws BookStoreException {
        Set<StockBook> booksToAdd = new HashSet<StockBook>();
        booksToAdd.add(getDefaultBook());
        storeManager.addBooks(booksToAdd);
        storeManager.releaseAllLocks();
    }

    /**
     * Method to clean up the book store, execute after every test case is run.
     *
     * @throws BookStoreException
     *             the book store exception
     */
    @After
    public void cleanupBooks() throws BookStoreException {
        storeManager.removeAllBooks();
        storeManager.releaseAllLocks();
    }


    /**
     * Test1 check whether the initial book copies before test is equal to the latter book copies after test or not
     *
     * @throws BookStoreException
     * @throws InterruptedException
     */
    @Test
    public void testMultiThread() throws BookStoreException, InterruptedException {
        // Set of books to buy
        Set<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, NUM_COPIES));

        List<StockBook> listBooksBeforeTest = storeManager.getBooks();
        storeManager.releaseAllLocks();

        Thread C1 = new Thread(() -> {
            try {
                client.buyBooks(booksToBuy);
                client.releaseAllLocks();
            } catch (BookStoreException e) {
                e.printStackTrace();
            }
        });

        Thread C2 = new Thread(() -> {
            try {
                storeManager.addCopies(booksToBuy);
                storeManager.releaseAllLocks();
            } catch (BookStoreException e) {
                e.printStackTrace();
            }
        });

        C2.start();
        C1.start();

        try {
            C1.join();
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        try {
            C2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<StockBook> listBooksAfterTest = storeManager.getBooks();
        storeManager.releaseAllLocks();

        assertTrue(listBooksBeforeTest.get(0).getNumCopies() == listBooksAfterTest.get(0).getNumCopies());


    }

    /**
     *
     * Test2 Check the Thread2's result is consistent or not
     *
     * @throws BookStoreException
     * @throws InterruptedException
     */
    @Test
    public void testMultiThread2() throws BookStoreException, InterruptedException {
        // Set of books to buy
        Set<BookCopy> booksToBuy = new HashSet<BookCopy>();
        booksToBuy.add(new BookCopy(TEST_ISBN, 2));

        final List<StockBook>[] listBooks = new List[20];

        Thread C1 = new Thread(() -> {
            try {
                client.buyBooks(booksToBuy);
                storeManager.addCopies(booksToBuy);
                client.releaseAllLocks();

            } catch (BookStoreException e) {
                e.printStackTrace();
            }
        });

        Thread C2 = new Thread(() -> {
            try {
                for (int i = 0; i < listBooks.length; i++){
                    listBooks[i] = storeManager.getBooks();
                }
                storeManager.releaseAllLocks();

            } catch (BookStoreException e) {
                e.printStackTrace();
            }
        });

        C1.start();
        C2.start();


        try {
            C2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            C1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < listBooks.length; i ++)
        {
            System.out.print(String.format("%1$d book name: %2$s, book copies: %3$s\n", i, listBooks[i].get(0).getTitle(), listBooks[i].get(0).getNumCopies()));
            if (i < listBooks.length - 1) {
                assertTrue(listBooks[i].size() == listBooks[i+1].size());
                assertTrue(listBooks[i].get(0).getISBN() == listBooks[i+1].get(0).getISBN());
                assertTrue(listBooks[i].get(0).getNumCopies() == listBooks[i+1].get(0).getNumCopies());
            }
        }

    }

    /**
     * Tear down after class.
     *
     * @throws BookStoreException
     *             the book store exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws BookStoreException {
        storeManager.removeAllBooks();
        storeManager.releaseAllLocks();

        if (!localTest) {
            ((BookStoreHTTPProxy) client).stop();
            ((StockManagerHTTPProxy) storeManager).stop();
        }
    }
}
